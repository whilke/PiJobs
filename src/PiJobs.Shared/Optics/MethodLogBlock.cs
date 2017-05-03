using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class MethodLogBlock : IAsyncDisposable
    {
        private string _caller;
        public MethodLogBlock(string memberName = "")
        {
            _caller = memberName;
        }

        public Task StartAsync()
        {
            return ServiceResolver.Optics.Add(Create("START"));
        }

        private OpticsEvent Create(string @type)
        {
            OpticsEvent ev = new OpticsEvent();
            ev.Properties[SpineKeys.EVENT_TYPE] = "MTDLOG";
            ev.Properties["CALLER"] = _caller;
            ev.Properties["MTDTYPE"] = @type;
            return ev;
        }

        public Task DisposeAsync()
        {
            return ServiceResolver.Optics.Add( Create("STOP") );
        }
    }
}
