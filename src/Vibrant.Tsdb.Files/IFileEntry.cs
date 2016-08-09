using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Files
{
   public interface IFileEntry<TKey> : IEntry<TKey>
   {
      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
