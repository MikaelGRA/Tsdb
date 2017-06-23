using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TaggedReadResult<TEntry, TMeasureType>
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      public TaggedReadResult( TMeasureType measureType, TagCollection groupedTags, Sort sort, List<TEntry> entries )
      {
         MeasureType = measureType;
         GroupedTags = groupedTags;
         Sort = sort;
         Entries = entries;
      }

      public TMeasureType MeasureType { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public TagCollection GroupedTags { get; private set; }
   }
}
