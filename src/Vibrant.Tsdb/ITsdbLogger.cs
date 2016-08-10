using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITsdbLogger
   {
      void Debug( string message );

      void Info( string message );

      void Warn( string message );

      void Error( string message );

      void Fatal( string message );

      void Debug( Exception e, string message );

      void Info( Exception e, string message );

      void Warn( Exception e, string message );

      void Error( Exception e, string message );

      void Fatal( Exception e, string message );
   }
}
