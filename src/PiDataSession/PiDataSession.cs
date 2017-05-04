using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using PiJobs.Shared;
using PiJobs.Shared.Contracts;
using PiJobs.Shared.Jobs;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;

using DataDict = Microsoft.ServiceFabric.Data.Collections.
    IReliableDictionary<string, string>;
using Newtonsoft.Json;
using System.Fabric.Description;

namespace PiDataSession
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class PiDataSession : StatefulService, IPiDataSession
    {
        DataSession _session;
        Task _runningJob;
        public PiDataSession(StatefulServiceContext context)
            : base(context)
        { }

        public async Task Close()
        {
            var serviceUri = new Uri(ServiceURI.PiDataSession(_session));
            using (var fc = new FabricClient())
            {
               while(true)
               {
                    try
                    {
                        await
                            fc.ServiceManager
                            .DeleteServiceAsync(new DeleteServiceDescription(serviceUri),
                                TimeSpan.FromMinutes(5), CancellationToken.None);
                        break;
                    }
                    catch (TimeoutException)
                    {
                    }
                    await Task.Delay(1000);
               }
            }
        }

        public async Task<string> FetchData()
        {
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));
            using (var tx = StateManager.CreateTransaction())
            {
                var r = await data.TryGetValueAsync(tx, "data");
                if (r.HasValue)
                {
                    return r.Value;
                }
            }
            return null;
        }

        public async Task Init(DataSession session)
        {
            ValidateSession(session);
            await storeSession(session);
        }

        public async Task StartJob(JobRecord job)
        {
            await ServiceResolver.PiQueue(_session).UpdateStatus(_session, JobState.RUNNING);
            await RunJob(job);
        }

        private Task RunJob(JobRecord job)
        {
            //somehow another job is trying to start when one is already running
            if (_runningJob != null)
            {
                throw new InvalidProgramException($"job is already running");
            }

            //the job is kicked into a background task since we don't want to block here
            //and wait
            _runningJob = Task.Factory.StartNew(async () =>
            {
                await Task.Delay(1000);
                await finishJob();
            }, TaskCreationOptions.LongRunning);

            return Task.FromResult(0);
        }


        private async Task storeSession(DataSession session)
        {
            _session = session;
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));

            var json = JsonConvert.SerializeObject(_session);
            using (var tx = StateManager.CreateTransaction())
            {
                await data.SetAsync(tx, "session", json);
                await tx.CommitAsync();
            }
        }
        private async Task restoreSession()
        {
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));
            using (var tx = StateManager.CreateTransaction())
            {
                var r = await data.TryGetValueAsync(tx, "session");
                if (r.HasValue)
                {
                    _session = JsonConvert.DeserializeObject<DataSession>(r.Value);
                }
            }
        }

        private async Task storeJob(JobRecord job)
        {
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));
            var json = JsonConvert.SerializeObject(job);
            using (var tx = StateManager.CreateTransaction())
            {
                await data.SetAsync(tx, "job", json);
                await tx.CommitAsync();
            }
        }
        private async Task finishJob()
        {
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));
            using (var tx = StateManager.CreateTransaction())
            {
                await ServiceResolver.PiQueue(_session).UpdateStatus(_session, JobState.FINISHED);
                await data.TryRemoveAsync(tx, "job");
                await tx.CommitAsync();
            }
        }

        private async Task TryRestartJob()
        {
            var data = await StateManager.GetOrAddAsync<DataDict>(nameof(DataDict));
            JobRecord job = null;
            using (var tx = StateManager.CreateTransaction())
            {
                var r = await data.TryGetValueAsync(tx, "job");
                if (r.HasValue)
                {
                    job = JsonConvert.DeserializeObject<JobRecord>(r.Value);
                }
            }

            if (job != null)
            {
                await StartJob(job);
            }
        }


        private void ValidateSession(DataSession session)
        {
            if (_session == null) return;
            if (session != _session)
            {
                throw new InvalidOperationException($"{session.Id != _session.Id}");
            }
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]{
                new ServiceReplicaListener(
                    (context)
                    => new FabricTransportServiceRemotingListener(context,this))};
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            //check if we are recovering from a primary failure.
            await restoreSession();
            await TryRestartJob();
        }
    }
}
