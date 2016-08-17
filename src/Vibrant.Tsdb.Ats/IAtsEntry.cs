using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface IAtsEntry : IEntry
   {
      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
