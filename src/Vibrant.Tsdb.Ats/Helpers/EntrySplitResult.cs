using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   internal class EntrySplitResult<TKey, TEntry>
      where TEntry : IAtsEntry
   {
      private HashSet<TEntry> _uniqueEntries;
      private List<TEntry> _entries;

      public EntrySplitResult( TKey key, string id, string partitionKey )
      {
         Key = key;
         _uniqueEntries = new HashSet<TEntry>( new EntryEqualityComparer<TKey, TEntry>() );
      }

      public TKey Key { get; set; }

      public string Id { get; set; }

      public string PartitionKey { get; set; }

      public void Insert( TEntry entry )
      {
         _uniqueEntries.Add( entry );
      }

      public void Sort( Sort sort )
      {
         _entries = new List<TEntry>( _uniqueEntries );
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
