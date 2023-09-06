public class Event
{
    public string id { get; set; }
    public string reference { get; set; }
    public string batchId { get; set; }
    public string carrierParcelStatusDescription { get; set; }
    public int carrierParcelStatusId { get; set; }
    public string cmsDescription { get; set; }
    public int cmsId { get; set; }
    public string cmsParcelStatusDescription { get; set; }
    public int cmsParcelStatusId { get; set; }
    public string sourceCountry { get; set; }
    public int parcelId { get; set; }
    public string consignmentId { get; set; }
    public string statusAchievedDateTime { get; set; }
    public string cmsNotifiedDateTime { get; set; }
    public string systemNotifiedDateTime { get; set; }
    public string createdOn { get; set; }
    public string consignmentDirection { get; set; }
    public int carrierId { get; set; }
    public string carrierName { get; set; }
    public string carrierCode { get; set; }
    public int carrierServiceId { get; set; }
    public int carrierServiceCode { get; set; }
    public string carrierServiceName { get; set; }
    public int parcelStatusId { get; set; }
    public string parcelStatusDescription { get; set; }
}