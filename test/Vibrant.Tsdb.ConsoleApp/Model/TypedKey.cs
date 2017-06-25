using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.ConsoleApp.Entries;

namespace Vibrant.Tsdb.ConsoleApp.Model
{
   public class TypedKey : ITypedKey<BasicKey, MeasureType>
   {
      private static readonly Random Rng = new Random();
      private static readonly string[] Placements = new[] { "Inside", "Outside" };
      private static readonly string[] Owners = new[] { "ABC", "DEF" };

      private BasicKey _key;
      private MeasureType _measureType;

      public TypedKey( BasicKey key, MeasureType measureType )
      {
         _key = key;
         _measureType = measureType;
      }

      public BasicKey Key => _key;

      public MeasureType GetMeasureType()
      {
         return _measureType;
      }

      public string GetTagValue( string name )
      {
         switch( name )
         {
            case "Placement":
               return Placements[ Rng.Next( Placements.Length ) ];
            case "Owner":
               return Owners[ Rng.Next( Owners.Length ) ];
            default:
               throw new InvalidOperationException();
         }
      }
   }
}
