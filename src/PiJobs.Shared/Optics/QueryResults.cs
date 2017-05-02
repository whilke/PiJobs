using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class QueryResults<T>
    {
        public List<T> Items { get; set; }
        public long ContinueToken { get; set; }
    }
}
