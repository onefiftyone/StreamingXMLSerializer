using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using OneFiftyOne.Serialization.StreamingXMLSerializer;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            string filename = "../../test-file.xml";
            using (var ds = new StreamingDataSet())
            {
                ds.ReadXML(filename);

                int c = 0;
                int t = ds["ROADWAY"].Count;
                foreach (var dr in ds["ROADWAY"].AsEnumerable<ExpandoObject>())
                {
                    c++;
                }
            }

        }
    }
}
