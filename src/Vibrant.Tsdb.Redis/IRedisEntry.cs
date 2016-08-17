using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   public interface IRedisEntry : IEntry
   {
      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
