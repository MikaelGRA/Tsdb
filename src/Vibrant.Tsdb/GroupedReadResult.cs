using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class GroupedReadResult<TKey, TEntry>
     where TEntry : IEntry
   {
      public GroupedReadResult( string measureTypeName, IEnumerable<KeyValuePair<string, string>> group, Sort sort, List<TEntry> entries )
      {
         MeasureTypeName = measureTypeName;
         Group = group;
         Sort = sort;
         Entries = entries;
      }

      public string MeasureTypeName { get; private set; }
      
      public IEnumerable<KeyValuePair<string, string>> Group { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }
   }
}
