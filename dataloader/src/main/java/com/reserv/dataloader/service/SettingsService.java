package com.reserv.dataloader.service;

import com.reserv.dataloader.entity.Settings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class SettingsService {

    private final DataService dataService;

    @Autowired
    public SettingsService(DataService dataService) {
        this.dataService = dataService;
    }

    public Settings save(Settings s){
        List<Settings> settingsList = dataService.fetchAllData(Settings.class);
        Settings settings = null;
        if(settingsList != null && settingsList.isEmpty()) {
            settings = settingsList.get(0);
            s.setId(settings.getId());
            dataService.save(s);
            return s;
        }else{
            dataService.save(s);
            return s;
        }
    }

    public Settings fetch() {
        List<Settings> settingsList = dataService.fetchAllData(Settings.class);
        if (settingsList != null && settingsList.isEmpty()) {
            return settingsList.get(0);
        }
        return null;
    }
}
