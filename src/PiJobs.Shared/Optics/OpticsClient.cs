using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class OpticsClient
    {
        public async Task<List<OpticsEvent>> Query(string query, DateTime start, DateTime end)
        {
            long ctoken = 0;
            List<OpticsEvent> events = new List<OpticsEvent>();
            do
            {
                var results = await ServiceResolver.Optics.Query(query, start, end, ctoken);
                events.AddRange(results.Events);
                ctoken = results.ContinuationToken;
            } while (ctoken != 0);

            return events;
        }
    }
}
