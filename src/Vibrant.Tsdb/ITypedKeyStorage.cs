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

      Task<IEnumerable<ITypedKey<TKey, TMeasureType>>> GetTaggedKeysAsync( IEnumerable<TKey> keys );
   }
}
