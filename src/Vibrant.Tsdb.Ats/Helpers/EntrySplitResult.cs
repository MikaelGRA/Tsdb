using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   internal class EntrySplitResult<TKey, TEntry>
      where TEntry : IAtsEntry
   {
      private List<TEntry> _entries;

      public EntrySplitResult( TKey key, string partitionKey )
      {
         Key = key;
         _entries = new List<TEntry>();
      }

      public TKey Key { get; set; }

      public string PartitionKey { get; set; }

      public void Insert( TEntry entry )
      {
         _entries.Add( entry );
      }

      public void Sort( Sort sort )
      {
         _entries.Sort( EntryComparer.GetComparer<TKey, TEntry>( sort ) );
      }

      public List<TEntry> Entries
      {
         get
         {
            return _entries;
         }
      }

      public DateTime From
      {
         get
         {
            return _entries[ 0 ].GetTimestamp();
         }
      }

      public DateTime To
      {
         get
         {
            return _entries[ _entries.Count - 1 ].GetTimestamp();
         }
      }
   }
}
