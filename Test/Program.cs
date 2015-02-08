using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using OneFiftyOne.Serialization.StreamingXMLSerializer;
using System.Data;

namespace Test
{
    class TestA
    {
        public string Prop1 { get; set; }
        public string Prop2 { get; set; }
        public int Prop3 { get; set; }

        public IEnumerable<TestB> ClassB { get; set; }
    }

    class TestB
    {
        public double Prop4 { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var t = new TestA { Prop1 = "bacon", Prop2 = "Tomato", Prop3 = 42 };
            var t2 = new TestA {
                Prop1 = "lettuce", Prop2 = "Olive", Prop3 = 421,
                ClassB = new List<TestB> { new TestB { Prop4 = 3.14 } }
            };

            var list = new List<TestA>();
            list.Add(t);
            list.Add(t2);
            StreamingXMLSerializer.Serialize(list, "../../TestA.xml");

            var list2 = StreamingXMLSerializer.DeserializeEnumerable<TestA>("../../TestA.xml");

            StreamingXMLSerializer.Serialize(list2, "../../TestA - out.xml");
        }
    }
}
