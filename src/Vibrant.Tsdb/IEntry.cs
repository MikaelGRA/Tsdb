using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IEntry
   {
      string GetId();

      void SetId( string id );

      DateTime GetTimestamp();

      byte GetTypeCode();

      void Write( BinaryWriter writer );

      void Read( BinaryReader reader );
   }
}
