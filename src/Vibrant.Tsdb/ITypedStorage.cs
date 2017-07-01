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
      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, AggregationFunction aggregationFunction, Sort sort = Sort.Descending );

      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, AggregationFunction aggregationFunction, Sort sort = Sort.Descending );

      Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, DateTime from, DateTime to, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, AggregationFunction aggregationFunction, Sort sort = Sort.Descending );
   }
}
