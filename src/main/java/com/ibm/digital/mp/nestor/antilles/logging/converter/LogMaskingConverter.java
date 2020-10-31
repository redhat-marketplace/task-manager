package com.ibm.digital.mp.nestor.antilles.logging.converter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import com.ibm.digital.mp.nestor.util.AesEncodingUtil;

@Plugin(name = "LogMaskingConverter", category = "Converter")
@ConverterKeys({ "mask" })
public class LogMaskingConverter extends LogEventPatternConverter
{
    private static final Logger logger = LogManager.getLogger(LogMaskingConverter.class);
    private static final String jsonReplacementRegexForAnonymize = "\"$1\":\"********\"";
    private static final String jsonKeysToAnonymize = "encode_[^\"]*|Email[^\"]*Address|[^\"]*Name[^\"]*|User[^\"]*Id|[^\"]*language[^\"]*"
            + "|[^\"]*Country[^\"]*|Org[^\"]*Name|[^\"]*Phone[^\"]*|[^\"]*Address[^\"]*|postal[^\"]*Code|[^\"]*state[^\"]*";
    private static final String jsonKeysToPseudonymize = "part[^\"]*Number";
    private static final String secret = "EmbVGIkZD4qxajAZ";

    protected LogMaskingConverter(String name, String style)
    {
        super(name, style);
    }

    @Override
    public void format(LogEvent event, StringBuilder outputMessage)
    {
        String inputMessage = event.getMessage().getFormattedMessage();
        ReadOnlyStringMap contextData = event.getContextData();
        String message = "";
        if (contextData != null && !contextData.isEmpty())
        {
            message = inputMessage + "\n" + contextData;
        }
        else
        {
            message = inputMessage;
        }
        String maskedMessage = message;
        try
        {
            maskedMessage = maskForAnonymize(message);
            maskedMessage = maskForPseudonymize(maskedMessage);
            maskedMessage = maskForEmailAddress(maskedMessage);
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            maskedMessage = message; // Although if this fails, it may be better to not log the message
        }
        outputMessage.append(maskedMessage);
    }

    private String maskForAnonymize(String message)
    {
        String pattern = "\"(" + jsonKeysToAnonymize + ")\":(\\s)*(\"([^\"]*)\")";
        Pattern jsonPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);

        StringBuffer buffer = new StringBuffer();
        Matcher matcher = jsonPattern.matcher(message);
        while (matcher.find())
        {
            matcher.appendReplacement(buffer, jsonReplacementRegexForAnonymize);

        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String maskForPseudonymize(String message) throws Exception
    {
        String pattern = "\"(" + jsonKeysToPseudonymize + ")\":(\\s)*\"(([^\"]*))\"";
        Pattern jsonPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);

        StringBuffer buffer = new StringBuffer();
        Matcher matcher = jsonPattern.matcher(message);

        while (matcher.find())
        {
            String jsonReplacementRegexForPseudonymize = "\"$1\":\"" + getPseudonymizedValue(matcher.group(3)) + "\"";
            matcher.appendReplacement(buffer, jsonReplacementRegexForPseudonymize);

        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String maskForEmailAddress(String message)
    {
        String pattern = "([A-Z0-9._%-]+@[A-Z0-9.-]+\\.[A-Z]{2,4})";
        Pattern emailPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);

        StringBuffer buffer = new StringBuffer();
        Matcher matcher = emailPattern.matcher(message);

        while (matcher.find())
        {
            String jsonReplacementForEmailAddress = "********";
            matcher.appendReplacement(buffer, jsonReplacementForEmailAddress);

        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String getPseudonymizedValue(String input) throws Exception
    {
        return AesEncodingUtil.encrypt(input, secret);

    }

    public static LogMaskingConverter newInstance()
    {
        return new LogMaskingConverter("LogMask", Thread.currentThread().getName());
    }

}
