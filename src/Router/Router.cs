using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using PiJobs.Shared.Contracts;
using PiJobs.Shared;

using SessionDict = Microsoft.ServiceFabric.Data.Collections.
    IReliableDictionary<PiJobs.Shared.DataSession, int>;
using AccountDict = Microsoft.ServiceFabric.Data.Collections.
    IReliableDictionary<string, bool>;
using System.Fabric.Description;

namespace RouterNS
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Router : StatefulService, IRouter
    {
        public Router(StatefulServiceContext context)
            : base(context)
        { }

        public async Task AddOrGet(DataSession session)
        {
            bool write = false;
            var sessions = await StateManager.GetOrAddAsync<SessionDict>(nameof(SessionDict));
            var accounts = await StateManager.GetOrAddAsync<AccountDict>(nameof(AccountDict));
            using (var tx = StateManager.CreateTransaction())
            {
                var accountChk = await accounts.TryGetValueAsync(tx, session.Account, LockMode.Update);
                if (!accountChk.HasValue)
                {
                    await CreateAccountServices(session);
                    await accounts.SetAsync(tx, session.Account, true);
                    write = true;
                }

                var chk = await sessions.TryGetValueAsync(tx, session, LockMode.Update);
                if (!chk.HasValue)
                {
                    await CreateDataService(session);
                    await sessions.SetAsync(tx, session, 1);
                    write = true;
                }

                if (write)
                {
                    await tx.CommitAsync();
                }
            }
        }

        public async Task Close(DataSession session)
        {
            //for POC we don't shutdown account services.
            var sessions = await StateManager.GetOrAddAsync<SessionDict>(nameof(SessionDict));
            using (var tx = StateManager.CreateTransaction())
            {
                var chk = await sessions.TryGetValueAsync(tx, session, LockMode.Update);
                if (chk.HasValue)
                {
                    await sessions.TryRemoveAsync(tx, session);
                    try
                    {
                        await session.PiDataSession().Close();
                    }
                    catch { }

                    await tx.CommitAsync();
                }
            }
        }

        private async Task CreateAccountServices(DataSession session)
        {
        
            StatefulServiceDescription sd = new StatefulServiceDescription();
            sd.ApplicationName = new Uri("fabric:/PiJobs");
            sd.ServiceName = new Uri(ServiceURI.PiQueue(session));
            sd.ServiceTypeName = "PiQueueType";
            sd.HasPersistedState = true;
            sd.MinReplicaSetSize = 2;
            sd.TargetReplicaSetSize = 3;
            sd.PartitionSchemeDescription = new SingletonPartitionSchemeDescription();

            using (FabricClient fc = new FabricClient())
            {
                await fc.ServiceManager
                    .CreateServiceAsync(sd, TimeSpan.FromSeconds(600), CancellationToken.None);
            }            
        }

        private async Task CreateDataService(DataSession session)
        {

            StatefulServiceDescription sd = new StatefulServiceDescription();
            sd.ApplicationName = new Uri("fabric:/PiJobs");
            sd.ServiceName = new Uri(ServiceURI.PiDataSession(session));
            sd.ServiceTypeName = "PiDataSessionType";
            sd.HasPersistedState = true;
            sd.MinReplicaSetSize = 2;
            sd.TargetReplicaSetSize = 3;
            sd.PartitionSchemeDescription = new SingletonPartitionSchemeDescription();

            using (FabricClient fc = new FabricClient())
            {
                await fc.ServiceManager
                    .CreateServiceAsync(sd, TimeSpan.FromSeconds(600), CancellationToken.None);
            }

            while(true)
            {
                try
                {
                    await session.PiDataSession().Init(session);
                    break;
                }
                catch { }
            }
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]{
                new ServiceReplicaListener(
                    (context)
                    => new FabricTransportServiceRemotingListener(context,this))};
        }

    }
}
