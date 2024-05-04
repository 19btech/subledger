package com.reserv.dataloader.controller;

import com.reserv.dataloader.entity.Settings;
import com.reserv.dataloader.service.DataService;
import com.reserv.dataloader.service.SettingsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dataloader/setting")
@Slf4j
public class SettingsController {

    private final SettingsService settingsService ;

    @Autowired
    public SettingsController(SettingsService settingsService) {
        this.settingsService = settingsService;
    }

    @PostMapping("/save")
    public void saveDate(@RequestBody Settings s) {
        settingsService.save(s);
    }
}
