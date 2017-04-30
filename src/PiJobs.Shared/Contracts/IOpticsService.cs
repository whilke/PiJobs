using Microsoft.ServiceFabric.Services.Remoting;
using PiJobs.Shared.Optics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Contracts
{
    public interface IOpticsService : IService
    {
        Task Add(OpticsEvent @event);
        Task Query();
    }
}
