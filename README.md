Hebrew 3-Gram prob calculator
===
## Authors
Yonathan Wolloch - 313231631
Elad Weindeld - 205648389

## How to Run 

1. create an s3 bucket for the project.
2. change the url in the next lines to fit your bucket name:
    - `Main:35`
    - `Main:44`
    - `Main:53`
    - `Main:75`
    - `FirstStep:127`
    - `FirstStep:131`
    - `SecondStep:76`
    - `SecondStep:77`
    - `SecondStep:81`
    - `ThirdStep:55`
    - `ThirdStep:56`
    - `ThirdStep:60` 
3. change the EC2 key-pair in `Main:67` to your key-pair
4. create 3 jars with different mains and upload them to your S3 bucket:
    - `FirstStep.main` called 3GramProb1.jar
    - `SecondStep.main` called 3GramProb2.jar
    - `ThirdStep.main` called 3GramProb3.jar
5. run `Main.main`

Statistics
---

Instances type: M4Large
Num of instances: 9
Total runtime: 53 minutes

Step 1: 
    - Step runtime: 22 minutes
    - Map input records: 163471963
    - Map output records: 954040832
    - Reduce input records: 954040832
    - Reduce output records: 596275521
Step 2: 
    - Step runtime: 19 minutes
    - Map input records: 596275521
    - Map output records: 596275521
    - Reduce input records: 596275521
    - Reduce output records: 2803960
 Step3:
    - Step runtime: 2 minutes
    - Map input records: 2803960
    - Map output records: 2803960

Analytics
---
> ____ אני הייתי:
> אני הייתי רוצה 0.0199
> אני הייתי אומר 0.0151
> אני הייתי צריך 0.0148
> אני הייתי . 0.0086
> אני הייתי יכול 0.0075

----
> ____ אני הולך:
> אני הולך בדרך 0.0154
> אני הולך . 0.0100
> אני הולך לבית 0.0093
> אני הולך אל 0.0086
> אני הולך למות 0.0052

----
> ____ חזר לארץ:
> חזר לארץ ישראל 0.0668
> חזר לארץ . 0.0454
> חזר לארץ ב 0.0041
> חזר לארץ בשנת 0.0036
> חזר לארץ עם 0.0018

----
> ____ חזר על:
> חזר על כל 0.0118
> חזר על כך 0.0052
> חזר על מה 0.0042
> חזר על זה 0.0039
> חזר על עצמו 0.0039

----
> ____ חיי כל:
> חיי כל אחד 0.0189
> חיי כל אדם 0.0089
> חיי כל בני 0.0062
> חיי כל איש 0.0040
> חיי כל החיים 0.0039

----
> ____ האמת היא:
> האמת היא לא 0.0250
> האמת היא . 0.0112
> האמת היא גם 0.0043
> האמת היא רק 0.0036
> האמת היא אחת 0.0023

----
> ____ הדבר הכי:
> הדבר הכי טוב 0.0255
> הדבר הכי גדול 0.0086
> הדבר הכי חשוב 0.0082
> הדבר הכי גרוע 0.0063
> הדבר הכי יפה 0.0050

----
> ____ היא אוהבת:
> היא אוהבת אותו 0.0545
> היא אוהבת אותך 0.0542
> היא אוהבת את 0.0501
> היא אוהבת . 0.0246
> היא אוהבת אותי 0.0238

----
> ____ להתאבל על:
> להתאבל על חורבן 0.0127
> להתאבל על ירושלים 0.0062
> להתאבל על מותו 0.0012
> להתאבל על מות 0.0012
> להתאבל על המת 0.0010

----
> ____ להאשים את:
> להאשים את כל 0.0225
> להאשים את עצמו 0.0055
> להאשים את היהודים 0.0051
> להאשים את ישראל 0.0026
> להאשים את עצמי 0.0019

As we can see the system got reasonable decisions for all cases.

---


