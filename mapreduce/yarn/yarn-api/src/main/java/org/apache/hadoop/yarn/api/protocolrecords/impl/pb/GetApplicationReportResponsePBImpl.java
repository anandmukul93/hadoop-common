package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProtoOrBuilder;


    
public class GetApplicationReportResponsePBImpl extends ProtoBase<GetApplicationReportResponseProto> implements GetApplicationReportResponse {
  GetApplicationReportResponseProto proto = GetApplicationReportResponseProto.getDefaultInstance();
  GetApplicationReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationReport applicationReport = null;
  
  
  public GetApplicationReportResponsePBImpl() {
    builder = GetApplicationReportResponseProto.newBuilder();
  }

  public GetApplicationReportResponsePBImpl(GetApplicationReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetApplicationReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationReport != null) {
      builder.setApplicationReport(convertToProtoFormat(this.applicationReport));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationReport getApplicationReport() {
    GetApplicationReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationReport != null) {
      return this.applicationReport;
    }
    if (!p.hasApplicationReport()) {
      return null;
    }
    this.applicationReport = convertFromProtoFormat(p.getApplicationReport());
    return this.applicationReport;
  }

  @Override
  public void setApplicationReport(ApplicationReport applicationMaster) {
    maybeInitBuilder();
    if (applicationMaster == null) 
      builder.clearApplicationReport();
    this.applicationReport = applicationMaster;
  }

  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto p) {
    return new ApplicationReportPBImpl(p);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }



}  