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

        public Task Query()
        {
            throw new NotImplementedException();
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
