package com.example;


import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class HelperKata {
    private static final  String EMPTY_STRING = "";
    private static String ANTERIOR_BONO = null;

    public static Flux<CouponDetailDto> getListFromBase64File(final String fileBase64) {

        try (InputStream inputStream = new ByteArrayInputStream(decodeBase64(fileBase64));
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        ) {
            return getCouponDetailDtoFlux(bufferedReader);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return Flux.empty();
    }

    private static Flux<CouponDetailDto> getCouponDetailDtoFlux(BufferedReader bufferedReader) {
        AtomicInteger counter = new AtomicInteger(0);
        String characterSeparated = FileCSVEnum.CHARACTER_DEFAULT.getId();
        Set<String> codes = new HashSet<>();
        return Flux.fromIterable(
                bufferedReader.lines().skip(1)
                        .map(line -> getTupleOfLine(line, line.split(characterSeparated), characterSeparated))
                        .map(tuple -> getCouponDetailDto(counter, codes, tuple)).collect(Collectors.toList())
        );
    }

    private static CouponDetailDto getCouponDetailDto(AtomicInteger counter, Set<String> codes, Tuple2<String, String> tuple) {
        String dateValidated = null;
        String errorMessage = null;
        String bonoForObject;
        String bonoEnviado;

        if (isBlankTuple(tuple)) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_COLUMN_EMPTY.toString();
        } else if (!codes.add(tuple.getT1())) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_CODE_DUPLICATE.toString();
        } else if (!validateDateRegex(tuple.getT2())) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_DATE_PARSE.toString();
        } else if (validateDateIsMinor(tuple.getT2())) {
            errorMessage = ExperienceErrorsEnum.FILE_DATE_IS_MINOR_OR_EQUALS.toString();
        } else {
            dateValidated = tuple.getT2();
        }

        bonoEnviado = tuple.getT1();
        bonoForObject = getBonoForObject(bonoEnviado);

        return CouponDetailDto.aCouponDetailDto()
                .withCode(bonoForObject)
                .withDueDate(dateValidated)
                .withNumberLine(counter.incrementAndGet())
                .withMessageError(errorMessage)
                .withTotalLinesFile(1)
                .build();
    }

    private static boolean isBlankTuple(Tuple2<String, String> tuple) {
        return tuple.getT1().isBlank() || tuple.getT2().isBlank();
    }

    private static String getBonoForObject(String bonoEnviado) {
        String bonoForObject = null;
        if (isNullOrEquals(bonoEnviado)) {
            ANTERIOR_BONO = typeBono(bonoEnviado);
            bonoForObject = bonoEnviado;
        }
        return bonoForObject;
    }

    private static boolean isNullOrEquals(String bonoEnviado) {
        return ANTERIOR_BONO == null || ANTERIOR_BONO.equals(typeBono(bonoEnviado));
    }

    public static String typeBono(String bonoIn) {
        String retorno;
        if (matchesBono(bonoIn)) {
            retorno= ValidateCouponEnum.EAN_13.getTypeOfEnum();
        }
        if (isBooleanReplaceAsteriscos(bonoIn)) {
            retorno= ValidateCouponEnum.EAN_39.getTypeOfEnum();

        } else {
            retorno= ValidateCouponEnum.ALPHANUMERIC.getTypeOfEnum();
        }
        return retorno;
    }

    private static boolean isBooleanReplaceAsteriscos(String bonoIn) {
        return bonoIn.startsWith("*")
                && lenghtAsteriscos(bonoIn);
    }

    private static boolean lenghtAsteriscos(String bonoIn) {
        return numberBiggestOrEqual(bonoIn.replace("*", "").length(),1)
                && numberLessOrEqual(bonoIn.replace("*", "").length(), 43);
    }

    private static boolean numberLessOrEqual(int num1, int num2){
        return num1 <= num2;
    }

    private static boolean numberBiggestOrEqual(int num1, int num2){
        return num1 >= num2;
    }

    private static boolean matchesBono(String bonoIn) {
        return bonoIn.chars().allMatch(Character::isDigit) && lenghtBono(bonoIn);
    }

    private static boolean lenghtBono(String bonoIn) {
        return numberBiggestOrEqual(bonoIn.length(), 12) && numberLessOrEqual(bonoIn.length(), 13);
    }

    public static boolean validateDateRegex(String dateForValidate) {
        String regex = FileCSVEnum.PATTERN_DATE_DEFAULT.getId();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(dateForValidate);
        return matcher.matches();
    }

    private static byte[] decodeBase64(final String fileBase64) {
        return Base64.getDecoder().decode(fileBase64);

    }

    private static Tuple2<String, String> getTupleOfLine(String line, String[] array, String characterSeparated) {
        return condicionprincipal(array)
                ? Tuples.of(EMPTY_STRING, EMPTY_STRING)
                : ternario2(line, array, characterSeparated);
    }

    private static boolean condicionprincipal(String[] array) {
        return Objects.isNull(array) || array.length == 0;
    }

    private static Tuple2<String, String> ternario2(String line, String[] array, String characterSeparated) {
        return array.length < 2
                ? ternario3(line, array, characterSeparated)
                : Tuples.of(array[0], array[1]);
    }

    private static Tuple2<String, String> ternario3(String line, String[] array, String characterSeparated) {
        return line.startsWith(characterSeparated)
                ? Tuples.of(EMPTY_STRING, array[0])
                : Tuples.of(array[0], EMPTY_STRING);
    }

    public static boolean validateDateIsMinor(String dateForValidate) {
        Boolean retorno = false;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FileCSVEnum.PATTERN_SIMPLE_DATE_FORMAT.getId());
            Date dateActual = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
            Date dateCompare = simpleDateFormat.parse(dateForValidate);
            retorno = numberLessOrEqual(dateCompare.compareTo(dateActual), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retorno;
    }
}
