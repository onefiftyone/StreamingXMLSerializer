using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using OneFiftyOne.Serialization.StreamingXMLSerializer;
using System.Data;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var dt = new StreamingDataTable("ROADWAY"))
            {
                dt.ReadXML("../../test-file-output-dt.xml");

                using (var dtout = new StreamingDataTable("ROADWAY"))
                {
                    dtout.DataSource = dt;
                    dtout.WriteXML("../../test-file-output-streaming-dt.xml");
                }
            }
        }
    }
}
