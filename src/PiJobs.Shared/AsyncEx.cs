using PiJobs.Shared.Optics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared
{
    public interface IAsyncDisposable
    {
        Task DisposeAsync();
    }

    public static class AsyncEx
    {
        public static async Task Using<T>(T resource, Func<T, Task> body)
            where T : IAsyncDisposable
        {
            try
            {
                await body(resource);
            }
            finally
            {
                await resource.DisposeAsync();
            }
        }

        public static async Task UsingWithLogger(string @class, Func<Task> body, [CallerMemberName]string caller="")
        {
            var resource = new MethodLogBlock($"{@class}.{caller}");
            await resource.StartAsync();
            try
            {
                await body();
            }
            finally
            {
                await resource.DisposeAsync();
            }
        }

        public static async Task<T> UsingWithLogger<T>(string @class, Func<Task<T>> body, [CallerMemberName]string caller = "")
        {
            var resource = new MethodLogBlock($"{@class}.{caller}");
            await resource.StartAsync();
            try
            {
                return await body();
            }
            finally
            {
                await resource.DisposeAsync();
            }
        }

    }
}
