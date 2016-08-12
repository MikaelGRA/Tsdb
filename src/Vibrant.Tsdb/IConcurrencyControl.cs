using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IConcurrencyControl
   {
      Task<IDisposable> ReadAsync();

      Task<IDisposable> WriteAsync();
   }
}
