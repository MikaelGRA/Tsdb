using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITaggableKeyStorage<TKey>
   {
      Task<IEnumerable<ITaggedKey<TKey>>> GetTaggedKeysAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<KeyValuePair<string, string>> tagsToDecorate );
   }
}
