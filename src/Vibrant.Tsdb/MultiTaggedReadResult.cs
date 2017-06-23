using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiTaggedReadResult<TEntry, TMeasureType> : IEnumerable<TaggedReadResult<TEntry, TMeasureType>>
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      private IDictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>> _results;

      public MultiTaggedReadResult( IDictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>> results )
      {
         _results = results;
      }

      public TaggedReadResult<TEntry, TMeasureType> FindResult( TagCollection tags )
      {
         return _results[ tags ];
      }

      public TaggedReadResult<TEntry, TMeasureType> FindResult( IEnumerable<KeyValuePair<string, string>> tags )
      {
         return _results[ new TagCollection( tags ) ];
      }

      public bool TryFindResult( TagCollection tags, out TaggedReadResult<TEntry, TMeasureType> readResult )
      {
         return _results.TryGetValue( tags, out readResult );
      }

      public bool TryFindResult( IEnumerable<KeyValuePair<string, string>> tags, out TaggedReadResult<TEntry, TMeasureType> readResult )
      {
         return _results.TryGetValue( new TagCollection( tags ), out readResult );
      }

      public IEnumerator<TaggedReadResult<TEntry, TMeasureType>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }
   }
}
