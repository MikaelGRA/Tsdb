using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   internal class EntrySplitResult<TEntry>
      where TEntry : IEntry
   {
      private List<TEntry> _entries;

      public EntrySplitResult( string id )
      {
         Id = id;
         
         _entries = new List<TEntry>();
      }

      public string Id { get; set; }

      public void Insert( TEntry entry )
      {
         _entries.Add( entry );
      }

      public void Sort( Sort sort )
      {
         _entries.Sort( EntryComparer.GetComparer<TEntry>( sort ) );
      }

      public IReadOnlyList<TEntry> Entries
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
            return _entries[ Entries.Count - 1 ].GetTimestamp();
         }
      }
   }
}
