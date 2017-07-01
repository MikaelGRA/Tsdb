using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITypedStorage<TEntry, TMeasureType>
      where TEntry : IAggregatableEntry
      where TMeasureType : IMeasureType
   {
      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<AggregatedField> fields, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, Sort sort = Sort.Descending );

      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<AggregatedField> fields, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, Sort sort = Sort.Descending );

      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<AggregatedField> fields, DateTime from, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, Sort sort = Sort.Descending );
   }
}
