package com.jd.binlog.server.common;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;
import org.apache.oro.text.regex.Perl5Matcher;

import java.util.Map;

/**
 * Created by hp on 14-9-3.
 */
public class RegexFunction extends AbstractFunction {

    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String pattern = FunctionUtils.getStringValue(arg1, env);
        String text = FunctionUtils.getStringValue(arg2, env);
        Perl5Matcher matcher = new Perl5Matcher();
        boolean isMatch = matcher.matches(text, PatternUtils.getPattern(pattern));
        return AviatorBoolean.valueOf(isMatch);
    }

    public String getName() {
        return "regex";
    }

}
