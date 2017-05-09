using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using StatefulService = Microsoft.ServiceFabric.Services.Runtime.StatefulService;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using Newtonsoft.Json;
using System.IO.Compression;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using PiJobs.Shared.Optics;
using PiJobs.Shared.Contracts;

namespace PiOptics
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class OpticsService : StatefulService, IOptics
    {
        private const string _INFO_CONFIG_FLAG = "Optics.EnableInfo";
        private static bool _enableInfoLogging = true;


        object sync = new object();
        OpticsSet _optics;
        Queue<OpticsEvent> queuedEvents = new Queue<OpticsEvent>();

        public OpticsService(StatefulServiceContext context)
            : base(context)
        {
        }

        static OpticsService()
        {
            try
            {
                var enableInfoLoggingStr = ConfigurationManager.AppSettings[_INFO_CONFIG_FLAG];
                if (!string.IsNullOrEmpty(enableInfoLoggingStr))
                {
                    _enableInfoLogging = bool.Parse(enableInfoLoggingStr);
                }
            }
            catch
            {

            }

        }

        public Task Add(OpticsEvent evt)
        {
            lock (sync)
            {
                if (!_enableInfoLogging && evt.GetProperty(SpineKeys.LOG_LEVEL) == "DEBUG")
                {
                }
                else
                {
                    queuedEvents.Enqueue(evt);
                }

            }
            return Task.FromResult(0);
        }

        public Task AddBatch(List<OpticsEvent> evts)
        {
            lock(sync)
            {
                foreach(var evt in evts)
                {
                    if (!_enableInfoLogging && evt.GetProperty(SpineKeys.LOG_LEVEL) == "DEBUG")
                    {
                    }
                    else
                    {
                        queuedEvents.Enqueue(evt);
                    }
                }
            }
            return Task.FromResult(0);
        }

        public async Task<OpticsResult> Query(string query, DateTime startTime, DateTime endTime, long continuationToken)
        {
            Stopwatch sw = Stopwatch.StartNew();
            try
            {

                var evts =  await _optics.Query(query, startTime, endTime, 800, continuationToken);

                if (evts.Count > 800)
                {
                    var ord = evts.OrderBy(e => e.Timestamp).ThenBy(e=> e.Id);
                    var ev = ord.Last();
                    var ctoken = OpticsSet.ToLong(ev.Id, ev.bucketId);

                    OpticsResult result = new OpticsResult();
                    result.Events = ord.ToList();
                    result.ContinuationToken = ctoken;
                    result.ExecutionTime = sw.Elapsed.TotalMilliseconds;
                    return result;
                }
                else
                {
                    OpticsResult result = new OpticsResult();
                    result.Events = evts;
                    result.ContinuationToken = 0;
                    result.ExecutionTime = sw.Elapsed.TotalMilliseconds;
                    return result;
                }
            }
            catch (Exception e)
            {
                OpticsResult result = new OpticsResult();
                result.Events = new List<OpticsEvent>();
                result.ContinuationToken = 0;
                result.ExecutionTime = sw.Elapsed.TotalMilliseconds;
                result.Error = e.ToString();
                return result;
            }
            finally
            {
            }
        }

        public async Task<int> QueryCount(string query, DateTime startTime, DateTime endTime)
        {
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                return await _optics.QueryCount(query, startTime, endTime);
            }
            catch
            {
                return 0;
            }
            finally
            {
            }
        }

        public Task Purge()
        {
            return _optics.Purge();
        }

        private void _pull_from_queue_()
        {
            while(true)
            {
                try
                {
                    if (queuedEvents.Count > 0)
                    {
                        List<OpticsEvent> shortList = new List<OpticsEvent>();
                        lock(sync)
                        {
                            for (int i = 0; i < 2000; ++i)
                            {
                                if (queuedEvents.Count > 0)
                                {
                                    OpticsEvent e = queuedEvents.Dequeue();
                                    shortList.Add(e);
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                        if (shortList.Count > 0)
                        {
                            _optics.AddRange(shortList).GetAwaiter().GetResult();
                        }
                    }
                }
                catch 
                {
                }
                               
                System.Threading.Thread.Sleep(10);
            }
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            var serviceReplicatListener = new[]
            {
                new ServiceReplicaListener(context => this.CreateServiceRemotingListener(context))
            };
            return serviceReplicatListener;
        }


        private Thread _pullQueueThread;
        /// <summary>
        ///     This is the main entry point for your service's partition replica.
        ///     RunAsync executes when the primary replica for this partition has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric terminates this partition's replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {

            var optics = new OpticsSet(StateManager, this);
            //we create into a temp object to allow rebuilding to fully finish before setting the object.
            _optics = optics;

            _pullQueueThread = new Thread(_pull_from_queue_);
            _pullQueueThread.IsBackground = true;
            _pullQueueThread.Start();

            DateTime cleanup_time = DateTime.UtcNow + TimeSpan.FromHours(1);
            while (!cancellationToken.IsCancellationRequested)
            {
                DateTime now = DateTime.Now;
                if (now > cleanup_time)
                {
                    cleanup_time = DateTime.UtcNow + TimeSpan.FromHours(1);
                    await _optics.Dropdata();
                }

                // Pause for 1 second before continue processing.
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
            }
        }

    }
}

