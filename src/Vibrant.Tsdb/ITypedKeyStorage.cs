using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITypedKeyStorage<TKey, TMeasureType>
      where TMeasureType : IMeasureType
   {
      Task<IEnumerable<ITypedKey<TKey, TMeasureType>>> GetTaggedKeysAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags );
   }

   public interface IAggregateFunctions<TEntry, TMeasureType>
      where TMeasureType : IMeasureType
   {
      TEntry Sum( IEnumerable<TEntry> entries, TMeasureType measureType );

      TEntry Average( IEnumerable<TEntry> entries, TMeasureType measureType );

      TEntry Min( IEnumerable<TEntry> entries, TMeasureType measureType );

      TEntry Max( IEnumerable<TEntry> entries, TMeasureType measureType );
   }
}
