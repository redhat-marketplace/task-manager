/********************************************************** {COPYRIGHT-TOP} ****
 * IBM Internal Use Only
 * IBM Marketplace
 *
 * (C) Copyright IBM Corp. 2017  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise  
 * divested of its trade secrets, irrespective of what has been 
 * deposited with the U.S. Copyright Office.
 ********************************************************** {COPYRIGHT-END} ***/

package com.ibm.digital.mp.nestor.antilles.webhooks;

public class WebhookAuthenticationException extends WebhookException
{
    private static final long serialVersionUID = 1L;

    private String errorCode;

    public WebhookAuthenticationException(String message)
    {
        super(message);
    }

    public WebhookAuthenticationException(String message, String errorCode)
    {
        super(message);
        setErrorCode(errorCode);
    }

    public WebhookAuthenticationException(Throwable cause)
    {
        super(cause);
    }

    public WebhookAuthenticationException(Throwable cause, String errorCode)
    {
        super(cause);
        setErrorCode(errorCode);
    }

    public WebhookAuthenticationException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public WebhookAuthenticationException(String message, Throwable cause, String errorCode)
    {
        super(message, cause);
        setErrorCode(errorCode);
    }

    public String getErrorCode()
    {
        return errorCode;
    }

    public void setErrorCode(String errorCode)
    {
        this.errorCode = errorCode;
    }
}
