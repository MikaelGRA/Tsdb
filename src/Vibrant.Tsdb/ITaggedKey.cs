using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITaggedKey<TKey>
   {
      TKey Key { get; }

      string GetMeasurementType();

      string GetTagValue( string key );

      //IEnumerable<KeyValuePair<string, string>> GetMeasurementTags();
   }
}
