using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public interface IWorkProvider<TKey>
   {
      event Action<TsdbVolumeMoval<TKey>> MovalChangedOrAdded;

      event Action<TKey> MovalRemoved;

      Task<IEnumerable<TsdbVolumeMoval<TKey>>> GetAllMovalsAsync( DateTime now );

      Task<TsdbVolumeMoval<TKey>> GetMovalAsync( TsdbVolumeMoval<TKey> completedMoval );

      TimeSpan GetTemporaryMovalInterval();

      int GetTemporaryMovalBatchSize();

      int GetDynamicMovalBatchSize();
   }
}
