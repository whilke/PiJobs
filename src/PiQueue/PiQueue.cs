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

namespace PiQueue
{
    internal sealed class PiQueue : StatefulService, IPiQueueService
    {
        private bool _poolBlocked = false;

        public PiQueue(StatefulServiceContext context)
            : base(context)
        { }

        public async Task<int> GetQueueSize()
        {
            if (!_poolBlocked) return 0;

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
                using (var tx = StateManager.CreateTransaction())
                {
                    job.JobState = JobState.QUEUED;
                    await queue.EnqueueAsync(tx, job);
                    await lookupDict.SetAsync(tx, session, job);
                    await tx.CommitAsync();
                }
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
                    var query = await lookupDict.TryGetValueAsync(tx, session, LockMode.Update);
                    if (query.HasValue)
                    {
                        var record = query.Value;
                        if (record.JobState != newStatus)
                        {
                            record.JobState = newStatus;
                            await lookupDict.SetAsync(tx, session, record);

                            if (newStatus == JobState.FINISHED)
                            {
                                await ReturnResources(tx, record);
                            }

                            await tx.CommitAsync();
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
                    var query = await lookupDict.TryGetValueAsync(tx, session, LockMode.Update);
                    if (query.HasValue)
                    {
                        await lookupDict.TryRemoveAsync(tx, session);
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

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            while(true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(1, cancellationToken);
                try
                {
                    await CheckAndExecJob();
                }
                catch { }
            }
        }

        private async Task CheckAndExecJob()
        {
            var queue = await StateManager.GetOrAddAsync<JobQueue>(nameof(JobQueue));
            using (var tx = StateManager.CreateTransaction())
            {
                if (await queue.GetCountAsync(tx) > 0)                    
                {
                    if (!await hasCapacity(tx))
                    {
                        _poolBlocked = true;
                        return;
                    }

                    var job = await queue.TryDequeueAsync(tx);
                    if (job.HasValue)
                    {
                        _poolBlocked = false;
                        await LockResources(tx, job.Value);

                        //send the job off to start running
                        //note: other end must return quickly to not block trans.
                        await job.Value.Session.PiDataSession().StartJob(job.Value);
                        await tx.CommitAsync();
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
            var r = await resources.TryGetValueAsync(tx, "bucket", LockMode.Update);

            return r.HasValue ? r.Value < bucket_size : true;
        }

        private async Task LockResources(ITransaction tx, JobRecord record)
        {
            var resources = await StateManager
                            .GetOrAddAsync<FirmResources>(tx, nameof(FirmResources));

            await resources.AddOrUpdateAsync(tx, "bucket", record.Cost,
                (k, v) => v + record.Cost);
        }

        private async Task ReturnResources(ITransaction tx, JobRecord record)
        {
            var resources = await StateManager
                .GetOrAddAsync<FirmResources>(tx, nameof(FirmResources));

            await resources.AddOrUpdateAsync(tx, "bucket", 0, 
                (k, v) => Math.Max(0, v - record.Cost));
        }
    }
}
