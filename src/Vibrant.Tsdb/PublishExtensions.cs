using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class PublishExtensions
   {
      public static Task PublishAsync<TKey, TEntry>( this IPublish<TKey, TEntry> publish, ISerie<TKey, TEntry> serie, PublicationType publicationType )
         where TEntry : IEntry
      {
         return publish.PublishAsync( new[] { serie }, publicationType );
      }
   }
}
