using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface IAtsEntry<TKey> : IEntry<TKey>
   {
      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
