using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITypedKey<TKey, TMeasureType>
      where TMeasureType : IMeasureType
   {
      TKey Key { get; }

      TMeasureType GetMeasureType();

      string GetTagValue( string key );

      IEnumerable<KeyValuePair<string, string>> GetAllTags();
   }
}
