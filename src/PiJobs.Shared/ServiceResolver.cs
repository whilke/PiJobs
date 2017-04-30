using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using PiJobs.Shared.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared
{
    public static class ServiceResolver
    {

        public static IOpticsService Optics =>
            ServiceProxy.Create<IOpticsService>
            (new Uri(ServiceURI.Optics), ServicePartitionKey.Singleton);
    }
}
