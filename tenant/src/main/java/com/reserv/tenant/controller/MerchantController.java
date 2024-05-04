package com.reserv.tenant.controller;

import com.reserv.tenant.entity.Merchant;
import com.reserv.tenant.service.MerchantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@Controller
@RequestMapping("/api/admin")
public class MerchantController {

    @Autowired
    MerchantService merchantService;
    @RequestMapping(value = "/custom", method = RequestMethod.GET)
    @ResponseBody
    public Mono<Merchant> getMerchant(@RequestParam String merchantId) {

        return merchantService.getMerchant(merchantId);
    }

    @RequestMapping(value = "/all", method = RequestMethod.GET)
    @ResponseBody
    public Mono<Merchant> getMerchants() {

        Merchant merchantOne = new Merchant();
        merchantOne.setMerchantName("M1");
        merchantOne.setAddress("Karachi");
        merchantOne.setEmail("m1@m1.com");
        merchantOne.setContactPerson("Ahmed");
        merchantOne.setWebDomain("MyMerchant.com");
        return merchantService.addMerchant(merchantOne);
    }
}
