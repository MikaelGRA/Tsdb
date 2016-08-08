using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IWorkProvider
   {
      event Action<TsdbVolumeMoval> MovalChangedOrAdded;

      event Action<string> MovalRemoved;

      Task<IEnumerable<TsdbVolumeMoval>> GetAllMovalsAsync( DateTime now );

      Task<TsdbVolumeMoval> GetMovalAsync( TsdbVolumeMoval completedMoval );

      TimeSpan GetTemporaryMovalInterval();

      int GetTemporaryMovalBatchSize();
   }
}
