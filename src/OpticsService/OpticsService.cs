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
using PiJobs.Shared.Optics;

using OpticsCollection =
    Microsoft.ServiceFabric.Data.Collections
    .IReliableDictionary<string, PiJobs.Shared.Optics.OpticsEvent>;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;

namespace OpticsService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class OpticsService : StatefulService, IOpticsService
    {
        OpticsSettings _settings;
        public OpticsService(StatefulServiceContext context)
            : base(context)
        {

            _settings = 
                new OpticsSettings(
                    context.CodePackageActivationContext
                    .GetConfigurationPackageObject("Config"));

        }

        public async Task Add(OpticsEvent @event)
        {
            long eventTicks = DateTime.UtcNow.Ticks;
            string eventGuid = Guid.NewGuid().ToString();
            string eventKey = $"{eventTicks}:{eventGuid}";
            string bucketKey = GenerateBucketKey(@event);

            var bucket = await StateManager.GetOrAddAsync<OpticsCollection>(bucketKey);
            using (var tx = StateManager.CreateTransaction())
            {
                await bucket.SetAsync(tx, eventKey, @event);
                await tx.CommitAsync();
            }
        }

        public async Task<QueryResults<OpticsEvent>> Query(DateTime start, DateTime end, List<KeyValuePair<string, string>> query, long ContinueToken)
        {
            //fast forward start to continue token if set.
            if (ContinueToken != 0)
            {
                start = new DateTime(ContinueToken);
            }

            var allBuckets = await GetBucketList();
            var buckets = allBuckets.Where(e =>
            {
                var dt = DateTime.Parse(e);
                return (dt >= start && dt <= end);
            });

            QueryResults<OpticsEvent> results = new QueryResults<OpticsEvent>()
            {
                Items = new List<OpticsEvent>()
            };
            foreach (var bucket in buckets)
            {
                var chk = await StateManager.TryGetAsync<OpticsCollection>(bucket);
                if (!chk.HasValue) continue;
                var rc = chk.Value;
                List<Tuple<long, OpticsEvent>> optics = new List<Tuple<long, OpticsEvent>>();
                using (var tx = StateManager.CreateTransaction())
                {
                    var iter = (await rc.CreateEnumerableAsync(tx)).GetAsyncEnumerator();
                    while(await iter.MoveNextAsync(CancellationToken.None))
                    {
                        var eventKey = iter.Current.Key;
                        var @event = iter.Current.Value;

                        var parts = eventKey.Split(':');
                        var ticks = long.Parse(parts[0]);
                        if (ContinueToken != 0 && ticks < ContinueToken)
                        {
                            continue;
                        }

                        optics.Add(new Tuple<long, OpticsEvent>(ticks, @event));
                    }
                }

                optics = optics.OrderBy(e => e.Item1).ToList();
                foreach(var optic in optics)
                {
                    var ticks = optic.Item1;
                    var @event = optic.Item2;
                    if (@event.Timestamp >= start
                                               && @event.Timestamp <= end
                                               && QueryPass(query, @event))
                    {
                        if (results.Items.Count > _settings.QuerySize)
                        {
                            results.ContinueToken = ticks;
                            return results;
                        }
                        results.Items.Add(@event);
                    }
                } 
            }
            return results;
        }

        private bool QueryPass(List<KeyValuePair<string,string>> query, OpticsEvent @event)
        {

            foreach(var kvp in query)
            {
                var key = kvp.Key;
                var @value = kvp.Value;

                if (@event.Properties.ContainsKey(key))
                {
                    return @event.Properties[key] == @value;
                }
                else
                {
                    return false;
                }
            }

            return true;
        }

        private async Task<List<string>> GetBucketList()
        {
            List<string> buckets = new List<string>();
            var iter = StateManager.GetAsyncEnumerator();
            while(await iter.MoveNextAsync(CancellationToken.None))
            {
                var rc = iter.Current;
                buckets.Add(rc.Name.ToString());
            }
            return buckets;

        }

       
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]{
                new ServiceReplicaListener(
                    (context) 
                    => new FabricTransportServiceRemotingListener(context,this))};
        }

        private string GenerateBucketKey(OpticsEvent @event)
        {
            return @event.Timestamp.ToString(_settings.Grain);           
        }

    }
}
