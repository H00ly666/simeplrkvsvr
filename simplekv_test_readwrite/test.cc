#include <assert.h>
#include <stdio.h>
#include <string>
#include <random>
#include <thread>
#include <iostream>
#include <sstream>
#include "PEngine1.h"

using namespace simplekvsvr;
using namespace std;

int main() {
  /*打开引擎*/

    char *value;
    PEngine *engine1 = new PEngine("./log");

    //64线程
    std::thread t[8];
    for (int i = 0; i < 8; i++) {
        t[i] = std::thread([i, engine1] {

    struct timeval start;
    struct  timeval end;
    unsigned long diff;
                string *a = new string[1000001];

                default_random_engine e;
                for(int i=0; i < 1000000; i++) {
                    a[i] = to_string(e()) + "wexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdfwexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdfwexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdf";

                }

                string  value = "1234567890wexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdfwexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdfwexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdfwexrcvtygbuyvctxrrctvybunibuivyctrxctvygbhinjobivyctxdrctfvybuhgvyfctdxrdcftyvgbuvyctxrctvybuqwertyuidtfyugihrextcyvubirxtfcyvguhbirxdtfcyvgubzerxdtcyvguxrtcyvubixtcyvubirxtcyvubitcyvgubitxcyvuberxtyvubszrxdtfyvurxetyvubrxtvgubrdxtyvubhjncudcudyvduddfvufvsdfdyvdfusvhdfvfdjvsfhdvjhdfvhdjfvsdvdsfvdfvhdfsjvdsjvhdfhvdf";

    //gettimeofday(&start,NULL);
                for(int n =0; n < 1000000; n++) {
                    engine1->put(a[n], value);
                }
    //gettimeofday(&end,NULL);


    //gettimeofday(&start,NULL);
            ///    string value__;
               // for(int n =0; n < 1000000; n++) {
                //    engine1->read(a[n], value__);
               // }
  //  gettimeofday(&end,NULL);

//diff = 1000000 * (end.tv_sec-start.tv_sec)+ end.tv_usec-start.tv_usec;

//    printf("thedifference is %ld\n",diff);
                delete [] a;
        });
    }

    for (auto &i : t)
        i.join();

    return 0;
}
