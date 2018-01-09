using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
{
   public interface ICosmosTablesEntry : IEntry
   {
      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
