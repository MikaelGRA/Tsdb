using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   public static class TableExtensions
   {
      public static DateTime ComputeFrom( this ITable table, DateTime originalFrom )
      {
         var tableFrom = table.From;
         if( originalFrom > tableFrom )
         {
            return originalFrom;
         }
         else
         {
            return tableFrom;
         }
      }

      public static DateTime ComputeTo( this ITable table, DateTime originalTo )
      {
         var tableTo = table.To;
         if( originalTo < tableTo )
         {
            return originalTo;
         }
         else
         {
            return tableTo;
         }
      }
   }
}
