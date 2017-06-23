using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITypedStorage<TKey, TEntry, TMeasureType> : IStorage<TKey, TEntry> 
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort = Sort.Descending );
   }
}
