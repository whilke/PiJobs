using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class OpticsEvent
    {
        public int bucketId = 0;
        public double DeedleField = 0;
        public int Id { get; set; }
        public string UniqueLogId { get; set; }
        public DateTime Timestamp { get; set; }
        public Dictionary<string, string> Properties { get; set; }
        public string Data { get; set; }

        public OpticsEvent()
        {
            UniqueLogId = Guid.NewGuid().ToString();
            Properties = new Dictionary<string, string>();
            Timestamp = DateTime.UtcNow;
        }

        public string GetProperty(string name)
        {
            if (Properties.ContainsKey(name))
                return Properties[name];
            else
                return "";
        }

        public Task Publish()
        {
            return ServiceResolver.Optics.Add(this);
        }
    }
}
