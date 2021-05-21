package com.example;


import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


public class HelperKata {
    private static final String EMPTY_STRING = "";
    private static String ANTERIOR_BONO = null;

    private HelperKata(){ }

    private static Flux<String> createFluxFrom(String fileBase64) {
        return Flux.using(
                () -> new BufferedReader(
                        new InputStreamReader(
                                new ByteArrayInputStream(decodeBase64(fileBase64))))
                        .lines().skip(1),
                Flux::fromStream,
                Stream::close
        );
    }

    public static Flux<CouponDetailDto> getListFromBase64File(final String fileBase64) {
        return getCouponDetailDtoFlux(createFluxFrom(fileBase64));
    }

    private static Flux<CouponDetailDto> getCouponDetailDtoFlux(Flux<String> fileFlux) {
        var counter = new AtomicInteger(0);
        String characterSeparated = FileCSVEnum.CHARACTER_DEFAULT.getId();
        Set<String> codes = new HashSet<>();
        return fileFlux
                .map(line -> getTupleOfLine(line, line.split(characterSeparated), characterSeparated))
                .map(tuple -> getCouponDetailDto(counter, codes, tuple));
    }

    private static CouponDetailDto getCouponDetailDto(AtomicInteger counter, Set<String> codes, Tuple2<String, String> tuple) {
        String dateValidated;
        String bonoForObject;
        String bonoEnviado;

        String errorMessage = messageError(codes, tuple);
        dateValidated = Optional.of(errorMessage)
                .filter(obj -> false)
                .map(el -> tuple.getT2())
                .orElse(null);

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

    private static String messageError(Set<String> codes, Tuple2<String, String> tuple) {
        Map<String, Boolean> map = new LinkedHashMap<>();
        map.put(ExperienceErrorsEnum.FILE_ERROR_COLUMN_EMPTY.toString(), isBlankTuple(tuple));
        map.put(ExperienceErrorsEnum.FILE_ERROR_CODE_DUPLICATE.toString(), !codes.add(tuple.getT1()));
        map.put(ExperienceErrorsEnum.FILE_ERROR_DATE_PARSE.toString(), !validateDateRegex(tuple.getT2()));
        map.put(ExperienceErrorsEnum.FILE_DATE_IS_MINOR_OR_EQUALS.toString(), validateDateIsMinor(tuple.getT2()));

        for (Map.Entry<String, Boolean> jugador : map.entrySet()) {
            if (jugador.getValue()) {
                return jugador.getKey();
            }
        }
        return null;
    }

    private static boolean isBlankTuple(Tuple2<String, String> tuple) {
        return tuple.getT1().isBlank() || tuple.getT2().isBlank();
    }

    private static String getBonoForObject(String bonoEnviado) {
        //Imperativo
        String bonoForObject = null;
        if (isNullOrEquals(bonoEnviado)) {
            ANTERIOR_BONO = typeBono(bonoEnviado);
            bonoForObject = bonoEnviado;
        }
        return bonoForObject;

        //Funcional
        /*
        return Optional.of(bonoEnviado)
            .filter(el->isNullOrEquals(el))
            .map(el->{
                ANTERIOR_BONO=typeBono(el);
                return el;
        }).orElse(null);
        */
    }

    private static boolean isNullOrEquals(String bonoEnviado) {
        return ANTERIOR_BONO == null || ANTERIOR_BONO.equals(typeBono(bonoEnviado));
    }

    public static String typeBono(String bonoIn) {
        return Optional.of(bonoIn)
                .filter(HelperKata::isBooleanReplaceAsteriscos)
                .map(el -> ValidateCouponEnum.EAN_39.getTypeOfEnum())
                .orElse(ValidateCouponEnum.ALPHANUMERIC.getTypeOfEnum());
    }

    private static boolean isBooleanReplaceAsteriscos(String bonoIn) {
        return bonoIn.startsWith("*")
                && lenghtAsteriscos(bonoIn);
    }

    private static boolean lenghtAsteriscos(String bonoIn) {
        return numberBiggestOrEqual(bonoIn.replace("*", "").length(), 1)
                && numberLessOrEqual(bonoIn.replace("*", "").length(), 43);
    }

    private static boolean numberLessOrEqual(int num1, int num2) {
        return num1 <= num2;
    }

    private static boolean numberBiggestOrEqual(int num1, int num2) {
        return num1 >= num2;
    }

    public static boolean validateDateRegex(String dateForValidate) {
        return dateForValidate.matches(FileCSVEnum.PATTERN_DATE_DEFAULT.getId());
    }

    private static byte[] decodeBase64(final String fileBase64) {
        return Base64.getDecoder().decode(fileBase64);
    }

    private static Tuple2<String, String> getTupleOfLine(String line, String[] array, String characterSeparated) {
        return isNullOrEmpty(array)
                ? Tuples.of(EMPTY_STRING, EMPTY_STRING)
                : arrayLength(line, array, characterSeparated);
    }

    private static boolean isNullOrEmpty(String[] array) {
        return Objects.isNull(array) || array.length == 0;
    }

    private static Tuple2<String, String> arrayLength(String line, String[] array, String characterSeparated) {
        return array.length < 2
                ? separatedEmptyString(line, array, characterSeparated)
                : Tuples.of(array[0], array[1]);
    }

    private static Tuple2<String, String> separatedEmptyString(String line, String[] array, String characterSeparated) {
        return line.startsWith(characterSeparated)
                ? Tuples.of(EMPTY_STRING, array[0])
                : Tuples.of(array[0], EMPTY_STRING);
    }

    public static boolean validateDateIsMinor(String dateForValidate) {
        var isMinor = false;
        try {
            var simpleDateFormat = new SimpleDateFormat(FileCSVEnum.PATTERN_SIMPLE_DATE_FORMAT.getId());
            var dateActual = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
            var dateCompare = simpleDateFormat.parse(dateForValidate);
            isMinor = numberLessOrEqual(dateCompare.compareTo(dateActual), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isMinor;
    }
}
