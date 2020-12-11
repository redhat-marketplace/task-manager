package com.ibm.digital.mp.nestor.antilles.webhooks;

public class TransformedTask
{

    private String type;
    private String referenceId;
    private String sequenceByReferenceId;
    private String sequenceByParentReferenceId;
    private String parentReferenceId;
    private String payload;
    private Long preferredExecutionDate;

    public Long getPreferredExecutionDate()
    {
        return preferredExecutionDate;
    }

    public void setPreferredExecutionDate(Long preferredExecutionDate)
    {
        this.preferredExecutionDate = preferredExecutionDate;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getReferenceId()
    {
        return referenceId;
    }

    public void setReferenceId(String referenceId)
    {
        this.referenceId = referenceId;
    }

    public String getSequenceByReferenceId()
    {
        return sequenceByReferenceId;
    }

    public void setSequenceByReferenceId(String sequenceByReferenceId)
    {
        this.sequenceByReferenceId = sequenceByReferenceId;
    }

    public String getSequenceByParentReferenceId()
    {
        return sequenceByParentReferenceId;
    }

    public void setSequenceByParentReferenceId(String sequenceByParentReferenceId)
    {
        this.sequenceByParentReferenceId = sequenceByParentReferenceId;
    }

    public String getParentReferenceId()
    {
        return parentReferenceId;
    }

    public void setParentReferenceId(String parentReferenceId)
    {
        this.parentReferenceId = parentReferenceId;
    }

    public String getPayload()
    {
        return payload;
    }

    public void setPayload(String payload)
    {
        this.payload = payload;
    }

}
