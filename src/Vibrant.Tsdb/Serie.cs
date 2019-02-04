using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class Serie<TKey, TEntry> : ISerie<TKey, TEntry>
     where TEntry : IEntry
   {
      public Serie( TKey key, IEnumerable<TEntry> entries )
      {
         Key = key;
         Entries = entries.ToList();
      }

      public Serie( TKey key, List<TEntry> entries )
      {
         Key = key;
         Entries = entries;
      }

      public Serie( TKey key, TEntry entry )
      {
         Key = key;
         Entries = new List<TEntry>();
         Entries.Add( entry );
      }

      public Serie( TKey key )
      {
         Key = key;
         Entries = new List<TEntry>();
      }

      public TKey Key { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public TKey GetKey()
      {
         return Key;
      }

      public List<TEntry> GetEntries()
      {
         return Entries;
      }
   }

   public class SortedSerie<TKey, TEntry> : Serie<TKey, TEntry>, ISortedSerie<TKey, TEntry>
     where TEntry : IEntry
   {
      private Sort _sort;

      public SortedSerie( TKey key, Sort sort ) : base( key )
      {
         _sort = sort;
      }

      public SortedSerie( TKey key, Sort sort, IEnumerable<TEntry> entries ) : base( key, entries )
      {
         _sort = sort;
      }

      public SortedSerie( TKey key, Sort sort, List<TEntry> entries ) : base( key, entries )
      {
         _sort = sort;
      }

      public SortedSerie( TKey key, Sort sort, TEntry entry ) : base( key, entry )
      {
         _sort = sort;
      }

      public Sort GetOrdering()
      {
         return _sort;
      }
   }
}
