using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Tests.Model
{
   public class TypedKey : ITypedKey<string, MeasureType>
   {
      private static readonly Random Rng = new Random();
      private static readonly string[] Placements = new[] { "Inside", "Outside" };
      private static readonly string[] Owners = new[] { "ABC", "DEF" };

      private string _key;
      private MeasureType _measureType;

      public TypedKey( string key, MeasureType measureType )
      {
         _key = key;
         _measureType = measureType;
      }

      public string Key => _key;

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
