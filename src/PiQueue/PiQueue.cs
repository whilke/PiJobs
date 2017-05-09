using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using PiJobs.Shared.Contracts;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using PiJobs.Shared;
using PiJobs.Shared.Jobs;

using JobLookupDict = Microsoft.ServiceFabric.Data.Collections.
    IReliableDictionary<PiJobs.Shared.DataSession, PiJobs.Shared.Jobs.JobRecord>;
using JobQueue = Microsoft.ServiceFabric.Data.Collections.
    IReliableQueue<PiJobs.Shared.Jobs.JobRecord>;
using FirmResources = Microsoft.ServiceFabric.Data.Collections.
    IReliableDictionary<string, int>;
using Microsoft.ServiceFabric.Data;
using PiJobs.Shared.Optics;

namespace PiQueue
{
    internal sealed class PiQueue : StatefulService, IPiQueueService
    {

        object lockObj = new object();
        int queueSize = 0;
        int runningSize = 0;

        public PiQueue(StatefulServiceContext context)
            : base(context)
        { }

        public async Task<Tuple<int, int,int>> GetStats()
        {
            return new Tuple<int, int, int>(5, await GetQueueSize(), await getCapacityUse());
        }
        public async Task<int> GetQueueSize()
        {
            var queue = await StateManager.GetOrAddAsync<JobQueue>(nameof(JobQueue));
            using (var tx = StateManager.CreateTransaction())
            {
                return (int)await queue.GetCountAsync(tx);
            }
        }

        public Task AddTask(DataSession session, string digits)
        {
            return AsyncEx.UsingWithLogger(nameof(PiQueue), async () =>
            {
                var queue = await StateManager.GetOrAddAsync<JobQueue>(nameof(JobQueue));
                var lookupDict = await StateManager.GetOrAddAsync<JobLookupDict>(nameof(JobLookupDict));

                var job = new JobRecord(session);
                job.Data = digits;

                OpticsEvent ev = new OpticsEvent();
                ev.Properties["TYPE"] = "JOB";
                ev.Properties["EVENT"] = "QUEUE";
                ev.Properties["STATE"] = "START";
                ev.Properties["SID"] = job.Id;
                ev.Properties["SESSION"] = job.Session.Id;
                ev.Properties["ACCOUNT"] = job.Session.Account;
                await ev.Publish();

                using (var tx = StateManager.CreateTransaction())
                {
                    job.JobState = JobState.QUEUED;
                    await queue.EnqueueAsync(tx, job, TimeSpan.FromSeconds(60), CancellationToken.None);
                    await lookupDict.SetAsync(tx, session, job, TimeSpan.FromSeconds(60), CancellationToken.None);
                    await tx.CommitAsync();
                }
                lock (lockObj) queueSize++;
            });       
        }

        public Task<JobState> GetStatus(DataSession session)
        {
            return AsyncEx.UsingWithLogger(nameof(PiQueue), async () =>
            {
                var lookupDict = await StateManager.GetOrAddAsync<JobLookupDict>(nameof(JobLookupDict));
                using (var tx = StateManager.CreateTransaction())
                {
                    var query = await lookupDict.TryGetValueAsync(tx, session);
                    if (query.HasValue)
                    {
                        return query.Value.JobState;
                    }
                }
                return JobState.INVALID;
            });            
        }

        public Task UpdateStatus(DataSession session, JobState newStatus)
        {
            return AsyncEx.UsingWithLogger(nameof(PiQueue), async () =>
            {
                var lookupDict = await StateManager.GetOrAddAsync<JobLookupDict>(nameof(JobLookupDict));
                using (var tx = StateManager.CreateTransaction())
                {
                    var query = await lookupDict.TryGetValueAsync(tx, session, LockMode.Update, TimeSpan.FromSeconds(60), CancellationToken.None);
                    if (query.HasValue)
                    {
                        var record = query.Value;
                        if (record.JobState != newStatus)
                        {
                            record.JobState = newStatus;
                            await lookupDict.SetAsync(tx, session, record, TimeSpan.FromSeconds(60), CancellationToken.None);

                            if (newStatus == JobState.FINISHED)
                            {
                                await ReturnResources(tx, record);
                            }

                            await tx.CommitAsync();

                            if (newStatus == JobState.FINISHED)
                            {
                                lock (lockObj)
                                    runningSize = Math.Max(runningSize-1, 0);
                            }
                        }
                    }
                }
            });
        }

        public Task RemoveStatus(DataSession session)
        {
            return AsyncEx.UsingWithLogger(nameof(PiQueue), async () =>
            {
                var lookupDict = await StateManager.GetOrAddAsync<JobLookupDict>(nameof(JobLookupDict));
                using (var tx = StateManager.CreateTransaction())
                {
                    var query = await lookupDict.TryGetValueAsync(tx, session, LockMode.Update, TimeSpan.FromSeconds(60), CancellationToken.None);
                    if (query.HasValue)
                    {
                        await lookupDict.TryRemoveAsync(tx, session, TimeSpan.FromSeconds(60), CancellationToken.None);
                        await tx.CommitAsync();
                    }
                }
            });
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]{
                new ServiceReplicaListener(
                    (context)
                    => new FabricTransportServiceRemotingListener(context,this))};
        }

        private Task pollTask;
        protected override Task RunAsync(CancellationToken cancellationToken)
        {
            lock(lockObj)
            {
                queueSize = GetQueueSize().GetAwaiter().GetResult();
                runningSize = getCapacityUse().GetAwaiter().GetResult();
            }
            pollTask = PollJobs(cancellationToken);
            return Task.FromResult(0);
        }

        private async Task PollJobs(CancellationToken cancellationToken)
        {
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Delay(200, cancellationToken);
                    try
                    {
                        await CheckAndExecJob();
                    }
                    catch { }
                }
            }
            catch
            {

            }
        }

        private async Task CheckAndExecJob()
        {
            var lookupDict = await StateManager.GetOrAddAsync<JobLookupDict>(nameof(JobLookupDict));
            var queue = await StateManager.GetOrAddAsync<JobQueue>(nameof(JobQueue));
            using (var tx = StateManager.CreateTransaction())
            {
                if (await queue.GetCountAsync(tx) > 0)                    
                {
                    if (!await hasCapacity(tx))
                    {
                        return;
                    }

                    var job = await queue.TryDequeueAsync(tx, TimeSpan.FromSeconds(60), CancellationToken.None);
                    if (job.HasValue)
                    {
                        await LockResources(tx, job.Value);

                        //send the job off to start running
                        //note: other end must return quickly to not block trans.
                        try
                        {
                            await job.Value.Session.PiDataSession().StartJob(job.Value);

                        }
                        catch
                        {
                            job.Value.JobState = JobState.FINISHED;
                            await lookupDict.SetAsync(tx, job.Value.Session, job.Value, TimeSpan.FromSeconds(60), CancellationToken.None);
                            await ReturnResources(tx, job.Value);
                        }
                        await tx.CommitAsync();
                        lock (lockObj)
                        {
                            runningSize++;
                            queueSize = Math.Max(queueSize - 1, 0);
                        }
                    }
                }
            }
        }

        private async Task<bool> hasCapacity(ITransaction tx)
        {
            //for moment hard code bucket size
            int bucket_size = 5;

            var resources = await StateManager
               .GetOrAddAsync<FirmResources>(nameof(FirmResources));
            var r = await resources.TryGetValueAsync(tx, "bucket", LockMode.Update, TimeSpan.FromSeconds(60), CancellationToken.None);

            return r.HasValue ? r.Value < bucket_size : true;
        }

        private async Task<int> getCapacityUse()
        {

            var resources = await StateManager
               .GetOrAddAsync<FirmResources>(nameof(FirmResources));
            using (var tx = StateManager.CreateTransaction())
            {
                var r = await resources.TryGetValueAsync(tx, "bucket", LockMode.Update, TimeSpan.FromSeconds(60), CancellationToken.None);
                return r.HasValue ? r.Value : 0;
            }

        }

        private async Task LockResources(ITransaction tx, JobRecord record)
        {
            var resources = await StateManager
                            .GetOrAddAsync<FirmResources>(tx, nameof(FirmResources));

            await resources.AddOrUpdateAsync(tx, "bucket", record.Cost,
                (k, v) => v + record.Cost, TimeSpan.FromSeconds(60), CancellationToken.None);
        }

        private async Task ReturnResources(ITransaction tx, JobRecord record)
        {
            var resources = await StateManager
                .GetOrAddAsync<FirmResources>(tx, nameof(FirmResources));

            await resources.AddOrUpdateAsync(tx, "bucket", 0, 
                (k, v) => Math.Max(0, v - record.Cost), TimeSpan.FromSeconds(60), CancellationToken.None);
        }
    }
}
