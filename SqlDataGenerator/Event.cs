public class Event
{
    public string Id { get; set; }
    public string Reference { get; set; }
    public string BatchId { get; set; }
    public string CarrierParcelStatusDescription { get; set; }
    public int CarrierParcelStatusId { get; set; }
    public string CmsDescription { get; set; }
    public int CmsId { get; set; }
    public string CmsParcelStatusDescription { get; set; }
    public int CmsParcelStatusId { get; set; }
    public string SourceCountry { get; set; }
    public int ParcelId { get; set; }
    public string ConsignmentId { get; set; }
    public string StatusAchievedDateTime { get; set; }
    public string CmsNotifiedDateTime { get; set; }
    public string SystemNotifiedDateTime { get; set; }
    public string CreatedOn { get; set; }
    public string ConsignmentDirection { get; set; }
    public int CarrierId { get; set; }
    public string CarrierName { get; set; }
    public string CarrierCode { get; set; }
    public int CarrierServiceId { get; set; }
    public int CarrierServiceCode { get; set; }
    public string CarrierServiceName { get; set; }
    public int ParcelStatusId { get; set; }
    public string ParcelStatusDescription { get; set; }
}