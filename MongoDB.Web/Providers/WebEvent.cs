using System;
using System.Web.Hosting;
using System.Web.Management;

namespace MongoDB.Web.Providers
{
    internal class WebEvent
    {
        public string ApplicationPath { get; set; }

        public string ApplicationVirtualPath { get; set; }

        public string Details { get; set; }
        
        public int EventCode { get; set; }
        
        public int EventDetailCode { get; set; }
        
        public Guid EventID { get; set; }
        
        public long EventOccurrence { get; set; }
        
        public long EventSequence { get; set; }
        
        public DateTime EventTime { get; set; }
        
        public DateTime EventTimeUtc { get; set; }
        
        public string EventType { get; set; }
        
        public string ExceptionType { get; set; }
        
        public string Message { get; set; }

        public static WebEvent FromWebBaseEvent(WebBaseEvent webBaseEvent)
        {
          var webEvent = new WebEvent
          {
            ApplicationPath = HostingEnvironment.ApplicationPhysicalPath,
            ApplicationVirtualPath = HostingEnvironment.ApplicationVirtualPath,
            Details = webBaseEvent.ToString(),
            EventCode = webBaseEvent.EventCode,
            EventDetailCode = webBaseEvent.EventDetailCode,
            EventID = webBaseEvent.EventID,
            EventOccurrence = webBaseEvent.EventOccurrence,
            EventSequence = webBaseEvent.EventSequence,
            EventTime = webBaseEvent.EventTime,
            EventTimeUtc = webBaseEvent.EventTimeUtc,
            EventType = webBaseEvent.GetType().Name,
            Message = webBaseEvent.Message
          };


          var baseErrorEvent = webBaseEvent as WebBaseErrorEvent;
          if (baseErrorEvent != null)
            {
                webEvent.ExceptionType = baseErrorEvent.ErrorException.GetType().Name;
            }

            return webEvent;
        }
    }
}