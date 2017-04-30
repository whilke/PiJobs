using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class OpticsEvent
    {
        public Dictionary<string, string> Properties { get; } =
            new Dictionary<string, string>();

        public DateTime Timestamp { get; } =
            DateTime.UtcNow;

        public OpticsEvent()
        {

        }

        public Task Publish()
        {
            return ServiceResolver.Optics.Add(this);
        }
    }
}
