using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class TaggedReadResult<TKey, TEntry> : ISerie<TKey, TEntry> 
      where TEntry : IEntry
   {
      internal TaggedReadResult( ITaggedKey<TKey> taggedId, Sort sort, List<TEntry> entries )
      {
         TaggedKey = taggedId;
         Sort = sort;
         Entries = entries;
      }

      public TKey Key => TaggedKey.Key;

      public ITaggedKey<TKey> TaggedKey { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public TKey GetKey()
      {
         return Key;
      }

      public ICollection<TEntry> GetEntries()
      {
         return Entries;
      }

      public TaggedReadResult<TKey, TEntry> MergeWith( TaggedReadResult<TKey, TEntry> other )
      {
         Entries = MergeSort.Sort(
            collections: new[] { Entries, other.Entries },
            comparer: EntryComparer.GetComparer<TKey, TEntry>( Sort ),
            resolveConflict: x => x.First() );

         return this;
      }
   }
}
